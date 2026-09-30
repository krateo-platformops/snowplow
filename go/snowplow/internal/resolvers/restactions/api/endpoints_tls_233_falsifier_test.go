//go:build unit
// +build unit

package api

// endpoints_tls_233_falsifier_test.go — #233.
//
// The CAPABILITY delegation (caPEMBytes could not parse an endpoint-owned CA)
// was SILENT; now it emits a BOUNDED one-shot WARN + bumps an uncapped OTLP
// detector counter. These arms drive the REAL httpClientForEndpoint boundary and
// keep the two delegation sites separately pinned (the #229 parity): the WARN is
// scoped to the capability site, never the routine POLICY site.

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

// rec233 captures the #233 WARN records (concurrency-safe for -race).
type rec233 struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (c *rec233) Enabled(context.Context, slog.Level) bool { return true }
func (c *rec233) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	c.recs = append(c.recs, r)
	c.mu.Unlock()
	return nil
}
func (c *rec233) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *rec233) WithGroup(string) slog.Handler      { return c }

// warnCount counts WARN records carrying the #233 message.
func (c *rec233) warnCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.recs {
		if r.Level == slog.LevelWarn && r.Message == "endpoints_tls.unparseable_ca_delegated" {
			n++
		}
	}
	return n
}

func install233Capture(t *testing.T) *rec233 {
	t.Helper()
	c := &rec233{}
	prev := slog.Default()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

func unparseableCAEndpoint(serverURL, ca string) *endpoints.Endpoint {
	return &endpoints.Endpoint{ServerURL: serverURL, Token: saTestToken, CertificateAuthorityData: ca}
}

func drive233(t *testing.T, ep *endpoints.Endpoint) {
	t.Helper()
	// warnUnparseableCADelegation fires during client CONSTRUCTION, before the
	// return, so we do not need to issue a request — we ignore the client/err.
	ri := &httpcall.RequestInfo{Verb: ptr.To(http.MethodGet)}
	_, _ = httpClientForEndpoint(context.Background(), ep, ri)
}

// TestIssue233_UnparseableCA_WarnsOnceAndCountsRate — the capability site: WARN
// one-shot per (ServerURL, CA-fingerprint), counter is the uncapped rate, and a
// CHANGED-but-still-bad CA re-warns (failed-fix visibility; a ServerURL-only key
// would fail arm 3).
func TestIssue233_UnparseableCA_WarnsOnceAndCountsRate(t *testing.T) {
	resetUnparseableCAStateForTest()
	rec := install233Capture(t)

	const srv = "https://apiserver.example:6443"
	const badCA = "!!! not pem and not base64 !!!"

	// Precondition: this endpoint IS the owned-CA shape → the CAPABILITY site,
	// not the POLICY site. (Pinned in both directions, per the #229 lesson.)
	if !endpointNeedsOwnedCAClient(unparseableCAEndpoint(srv, badCA)) {
		t.Fatal("precondition: the unparseable-CA endpoint must be owned-predicate=true (capability site)")
	}

	// (1) first unparseable-CA delegation → EXACTLY one WARN + counter=1.
	drive233(t, unparseableCAEndpoint(srv, badCA))
	if got := rec.warnCount(); got != 1 {
		t.Fatalf("#233: the first unparseable-CA delegation must WARN exactly once; got %d", got)
	}
	if got := UnparseableCADelegationTotal(); got != 1 {
		t.Fatalf("#233: counter must be 1 after one delegation; got %d", got)
	}

	// (2) SAME CA, same endpoint → NO 2nd WARN (bounded one-shot), counter=2 (rate).
	drive233(t, unparseableCAEndpoint(srv, badCA))
	if got := rec.warnCount(); got != 1 {
		t.Fatalf("#233: a repeat of the SAME bad CA must NOT re-WARN (one-shot per fingerprint); got %d WARNs", got)
	}
	if got := UnparseableCADelegationTotal(); got != 2 {
		t.Fatalf("#233: counter must be 2 — the uncapped RATE is independent of the one-shot log; got %d", got)
	}

	// (3) DIFFERENT bad CA, SAME endpoint → RE-WARN (failed-fix visibility) + counter=3.
	// A ServerURL-only one-shot key would suppress this — the discriminating arm.
	drive233(t, unparseableCAEndpoint(srv, "@@@ different but still bad @@@"))
	if got := rec.warnCount(); got != 2 {
		t.Fatalf("#233: a CHANGED-but-still-bad CA on the same endpoint must RE-WARN (fingerprint key = "+
			"failed-fix visibility; a ServerURL-only key would suppress it); got %d WARNs", got)
	}
	if got := UnparseableCADelegationTotal(); got != 3 {
		t.Fatalf("#233: counter must be 3; got %d", got)
	}
}

// TestIssue233_PolicyDelegation_StaysSilent — the scope control (#229 two-site
// distinction): a no-CA endpoint delegates at the POLICY site, which is routine
// and correctly SILENT. The #233 WARN + counter must NOT leak here.
func TestIssue233_PolicyDelegation_StaysSilent(t *testing.T) {
	resetUnparseableCAStateForTest()
	rec := install233Capture(t)

	const srv = "https://apiserver.example:6443"
	ep := &endpoints.Endpoint{ServerURL: srv, Token: saTestToken} // no CA → policy site
	if endpointNeedsOwnedCAClient(ep) {
		t.Fatal("precondition: a no-CA endpoint must be owned-predicate=false (policy site)")
	}

	drive233(t, ep)
	if got := rec.warnCount(); got != 0 {
		t.Fatalf("#233 SCOPE: the POLICY delegation must stay SILENT (no #233 WARN); got %d", got)
	}
	if got := UnparseableCADelegationTotal(); got != 0 {
		t.Fatalf("#233 SCOPE: the POLICY delegation must NOT bump the unparseable-CA counter; got %d", got)
	}
}
