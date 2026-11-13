// main_public_identity_logs_1334_test.go — #1334 MN5: the public identity
// reaches logs only as its class, never as a user name. A token-less request
// runs through main's request chain (newRequestChain, buildLogHandler) and the
// REAL auth middleware with the public identity on, then the real resolver,
// against an apiserver whose refusal names the impersonated user and group the
// way a real one does. No line carries "krateo:public" or
// "system:unauthenticated"; the refusal of an unavailable public identity is
// labelled identity_class=public.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/client-go/rest"
)

func publicIdentityAPI1334(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "Forbidden", "code": 403,
			"message": `pods is forbidden: User "krateo:public" cannot list resource "pods"; groups "system:unauthenticated" is forbidden`,
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func publicGrant1334(t *testing.T, granted bool) {
	t.Helper()
	cache.ResetImpersonationCapabilityForTest()
	t.Cleanup(cache.ResetImpersonationCapabilityForTest)
	t.Cleanup(cache.SetImpersonationAvailableForTest(true))
	cache.SetImpersonationReviewer(func(_ context.Context, resource, _, name string) (bool, error) {
		return granted && ((resource == cache.ImpersonateResourceUsers && name == middleware.PublicUsername) ||
			(resource == cache.ImpersonateResourceGroups && name == middleware.PublicGroup)), nil
	})
}

func runPublic1334(t *testing.T, pretty bool, granted bool) ([]string, int) {
	t.Helper()
	prevTM := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prevTM) })
	publicGrant1334(t, granted)
	api, _ := publicIdentityAPI1334(t)

	capture := &lockedBuffer{}
	log := slog.New(buildLogHandler(pretty, slog.LevelDebug, capture))
	prev := slog.Default()
	slog.SetDefault(log)
	t.Cleanup(func() { slog.SetDefault(prev) })

	public := middleware.NewPublicIdentity(
		func() (*endpoints.Endpoint, error) { return &endpoints.Endpoint{ServerURL: api.URL, Token: "sa-1334"}, nil },
		func() (*rest.Config, error) { return &rest.Config{Host: api.URL, BearerToken: "sa-1334"}, nil })
	work := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := xcontext.Logger(r.Context())
		l.Debug(probeMsg1192)
		l.Info(probeMsg1192)
		l.Warn(probeMsg1192)
		l.Error(probeMsg1192)
		restactionsapi.ResolveWithFailure(r.Context(), restactionsapi.ResolveOptions{
			RC:      &rest.Config{Host: api.URL},
			AuthnNS: "authn-ns",
			Items: []*templates.API{
				{Name: "denied", Verb: ptr.To(http.MethodGet), Path: "/api/v1/namespaces/default/pods", ErrorKey: ptr.To("deniedErr")},
			},
			RESTActionNamespace: "ns-1334",
			RESTActionName:      "ra-1334",
		})
		w.WriteHeader(http.StatusOK)
	})
	h := newRequestChain(log).Append(middleware.UserConfigOrPublic(jwtutil.NewStaticKeySource(nil), "authn-ns", public)).Then(work)
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Header.Set(xcontext.LabelKrateoTraceId, traceID1192)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return strings.Split(strings.TrimSpace(capture.String()), "\n"), rec.Code
}

func TestIssue1334_PublicIdentity_LogsCarryTheClassNeverTheName(t *testing.T) {
	for _, h := range handlers1192 {
		t.Run(h.name+"/served", func(t *testing.T) {
			lines, code := runPublic1334(t, h.pretty, true)
			if code != http.StatusOK {
				t.Fatalf("precondition: the public request reaches the handler; code=%d", code)
			}
			levels := map[string]bool{}
			for _, line := range lines {
				for _, tok := range []string{"krateo:public", "system:unauthenticated"} {
					if strings.Contains(line, tok) {
						t.Errorf("MUST NEVER: the public identity's name %q on a log line: %s", tok, line)
					}
				}
				rec := decodeLine1192(t, h.pretty, line)
				if rec.values["msg"] == probeMsg1192 {
					levels[rec.values["level"]] = true
				}
			}
			for _, l := range []string{"DEBUG", "INFO", "WARN", "ERROR"} {
				if !levels[l] {
					t.Errorf("CONTROL: no %s line of the public request reached the capture; lines=%v", l, lines)
				}
			}
		})
		t.Run(h.name+"/unavailable", func(t *testing.T) {
			lines, code := runPublic1334(t, h.pretty, false)
			if code != http.StatusServiceUnavailable {
				t.Fatalf("precondition: an unavailable public identity is 503; code=%d", code)
			}
			labelled := false
			for _, line := range lines {
				for _, tok := range []string{"krateo:public", "system:unauthenticated"} {
					if strings.Contains(line, tok) {
						t.Errorf("MUST NEVER: the public identity's name %q on a log line: %s", tok, line)
					}
				}
				rec := decodeLine1192(t, h.pretty, line)
				if strings.Contains(rec.values["msg"], "public identity unavailable") && rec.values["identity_class"] == "public" {
					labelled = true
				}
			}
			if !labelled {
				t.Errorf("the refusal must be logged with identity_class=public; lines=%v", lines)
			}
		})
	}
}
