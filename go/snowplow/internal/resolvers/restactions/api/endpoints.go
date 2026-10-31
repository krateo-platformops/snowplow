package api

import (
	"context"
	"fmt"
	"log/slog"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"k8s.io/client-go/rest"
)

type endpointReferenceMapper struct {
	authnNS  string
	username string
	// groups are the requester's groups. With username they choose the
	// caller's own credential Secret (credsecret.For, #994).
	groups []string
	rc     *rest.Config
	// provenance (#443 part 2) — under ProvenanceCallerSupplied a named
	// endpointRef Secret is read with the caller's own credentials.
	provenance Provenance
}

// The per-user credential Secret names come from credsecret (#994): For picks
// the one a validated identity reads (`<user>-clientconfig`, or
// `<user>-clientconfig-stepup` when its groups carry krateo:loa:2), and
// IsPerUserName is the reserved set the #1025 rule (refusePerUserRef) keys on. Single source, so
// the rule and the synthesis can never drift onto different strings
// (feedback_consultation_mutation_is_not_key_correctness). It is a general
// boundary on the reserved names, NOT a per-resource carve-out
// (feedback_no_special_cases).

// resolveOne resolves a named/nil endpoint Reference to an Endpoint.
//
// #1025 — its only caller, resolveStageEndpoint, has already passed the named
// ref through refusePerUserRef (per_user_ref.go), for every stage, UAF or
// not, so a per-user credential name never reaches the lookup below. The
// nil-ref synthesis legitimately builds the caller's OWN per-user name.
//
// resolveOne additionally reports isSAEndpoint: true ONLY on the
// InternalEndpointFromContext branch, which always carries snowplow's own SA
// endpoint (every WithInternalEndpoint attach passes saEP). This is the PROVENANCE
// signal the dial site uses to thread the SA token FILE (#267) — the SA endpoint is
// shape-identical to a token-auth per-user clientconfig, so it can only be
// distinguished by where it came from, never by field shape (arch-268 ruling). All
// other returns (wrong-type fall-through, clientconfig cache-hit, FromSecret,
// errors) report false.
func (m *endpointReferenceMapper) resolveOne(ctx context.Context, ref *templates.Reference) (endpoints.Endpoint, bool, error) {
	isInternal := false
	// #443 part 2 — a caller-supplied (inline draft) named ref is read as the
	// CALLER, never through the ServiceAccount or the SA-backed Secrets
	// snapshot, so a draft can only use credentials its author can already
	// read.
	if ref != nil && m.provenance == ProvenanceCallerSupplied {
		return m.resolveOneAsCaller(ctx, ref)
	}
	if ref == nil {
		// 0.30.102 Tag B: when the request is driven by an internal /
		// startup path (Phase 1's SA walk, or the background refresher /
		// prewarm re-resolve) the context carries an explicit internal-dispatch
		// endpoint via cache.WithInternalEndpoint. There is no `<user>-clientconfig`
		// Secret for the synthetic SA identity, so the per-user lookup
		// below would fail; consult the context-carried endpoint first.
		// LIVE per-user requests never set it — they fall through to the unchanged
		// clientconfig path. (This was briefly FALSE under the 0.30.166 attach,
		// which put the SA endpoint on every per-user /call — the #268/#269 leak
		// vector — and is TRUE again since Part 1 removed that attach.) General
		// mechanism, not a per-resource carve-out (feedback_no_special_cases.md).
		if v, ok := cache.InternalEndpointFromContext(ctx); ok {
			if ep, epOK := v.(*endpoints.Endpoint); epOK && ep != nil {
				// PROVENANCE: the context-carried internal endpoint is ALWAYS the SA
				// endpoint → isSAEndpoint=true. The dial site threads the SA token
				// FILE (#267) for it; a per-user clientconfig (resolved below via
				// FromSecret) is never marked, so it keeps its own static token.
				return *ep, true, nil
			}
			// The context carried an internal endpoint but it is not a
			// usable *endpoints.Endpoint (wrong shape, or a nil pointer).
			// Fall through to the per-user clientconfig path — but WARN:
			// an internal driver wired the wrong type and its dispatches
			// will silently take the per-user path, which has no
			// `<sa>-clientconfig` Secret and so will fail. Loud so a
			// future caller passing the wrong shape is diagnosable.
			xcontext.Logger(ctx).Warn("resolveOne: internal endpoint present but not a usable *endpoints.Endpoint; falling through to per-user clientconfig",
				slog.String("subsystem", "cache"),
				slog.String("got_type", fmt.Sprintf("%T", v)),
				slog.String("hint", "an internal driver (e.g. Phase 1) must pass *endpoints.Endpoint to cache.WithInternalEndpoint"),
			)
		}
		// #994 — the caller's OWN credential, chosen exactly as the auth
		// middleware chose it: the step-up Secret for an identity whose
		// groups carry krateo:loa:2, else the session Secret. No fallback: a
		// step-up identity whose Secret is gone fails this stage rather than
		// dialing the session credential.
		ref = &templates.Reference{
			Namespace: m.authnNS,
			Name:      credsecret.For(m.username, m.groups),
		}
		isInternal = true
	}

	// Ship D.2 (0.30.143) — F-3 cache lookup FIRST. The Secrets
	// snapshot covers AUTHN_NAMESPACE `<user>-clientconfig` Secrets
	// in-process; on a hit we skip the apiserver round-trip
	// entirely. On any soft miss (cache=off, namespace mismatch,
	// pre-sync, Secret absent, snapshot nil pre-readiness) the
	// function returns served=false and we fall through to the
	// upstream plumbing call below — the pre-D.2 behavior preserved
	// exactly (AC-D2.4). The Ship D RecordApiserverFallthrough call
	// is BELOW the cache-hit return so the F-3 counter measures
	// only the fallback path; the F-3 falsifier (134/60s → ≤5/60s)
	// reads that drop.
	if ep, served, ferr := FromInformerSecret(ctx, ref.Namespace, ref.Name); served {
		// Hard error path: Secret present but malformed
		// (server-url missing). Upstream FromSecret would have
		// returned the same error verbatim — we propagate it.
		if ferr != nil {
			return ep, false, ferr
		}
		// AC-D2.7 — the isInternal+!env.TestMode ServerURL override
		// applies UNIFORMLY on both the cache-hit and the upstream-
		// fallback path. Kept here (not factored into
		// FromInformerSecret) per PM-explicit `feedback_no_special_cases`:
		// the override is the resolver's contextual decision (the
		// stage is internal-driven), not a Secret-cache concern.
		if isInternal && !env.TestMode() {
			ep.ServerURL = "https://kubernetes.default.svc"
		}
		return ep, false, nil
	}

	// Ship D (0.30.141) — F-3: endpoints.FromSecret issues a per-user
	// clientconfig-Secret GET (apiserver) per non-UAF stage per /call —
	// architect's largest snowplow-attributable apiserver traffic
	// source on the request path. Record BEFORE the plumbing call so a
	// panicking secret-GET still increments the counter (AC-D.3). The
	// gvr label is left empty: the Secret being read is fixed per user,
	// not per resolver target, and synthesizing a placeholder would
	// inflate cardinality without diagnostic value.
	sctx := cache.RecordApiserverFallthrough(ctx, cache.ReasonSecretGet, "")
	ep, err := credhttp.FromSecret(sctx, m.rc, ref.Name, ref.Namespace)
	if err != nil {
		return ep, false, err
	}
	// Ship 0.30.165 — normalize CA bytes to the single-base64-encoded
	// PEM shape that plumbing's transport expects (see endpoints_ca.go).
	// Live `<user>-clientconfig` Secrets store double-base64 PEM; without
	// this normalization plumbing silently fails to populate RootCAs and
	// requests die with `x509: certificate signed by unknown authority`.
	ep.CertificateAuthorityData = string(normalizeCAData([]byte(ep.CertificateAuthorityData)))
	if isInternal && !env.TestMode() {
		ep.ServerURL = "https://kubernetes.default.svc"
	}

	return ep, false, nil
}
