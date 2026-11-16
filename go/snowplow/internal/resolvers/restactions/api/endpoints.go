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
// not, so a per-user credential name never reaches the lookup below.
// TestRev1032_ResolveOneOnlyBehindRefusePerUserRef keeps it that way: it
// fails on any other caller of resolveOne, and on a rule that no longer
// precedes every resolveOne call there. The nil-ref synthesis legitimately
// builds the caller's OWN per-user name.
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
			xcontext.Logger(ctx).WarnContext(ctx, "resolveOne: internal endpoint present but not a usable *endpoints.Endpoint; falling through to per-user clientconfig",
				slog.String("subsystem", "cache"),
				slog.String("got_type", fmt.Sprintf("%T", v)),
				slog.String("hint", "an internal driver (e.g. Phase 1) must pass *endpoints.Endpoint to cache.WithInternalEndpoint"),
			)
		}
		// #1334 — an identity the apiserver does not treat as authenticated
		// (the public identity) holds no clientconfig credential: the auth
		// middleware never reads one for it, and the name credsecret.For
		// would build is a REAL user's Secret (`krateo-public-clientconfig`
		// belongs to a user named krateo-public). Its own-credential stage
		// dials only through the impersonating context above; without it the
		// stage fails here, before any Secret is read.
		if !cache.Authenticated(m.username, m.groups) {
			return endpoints.Endpoint{}, false, fmt.Errorf("an unauthenticated identity has no clientconfig credential; its stage needs the impersonating context")
		}
		// #994 — the caller's OWN credential, chosen exactly as the auth
		// middleware chose it: the step-up Secret for an identity whose
		// groups carry krateo:loa:2, else the session Secret. No fallback: a
		// step-up identity whose Secret is gone fails this stage rather than
		// dialing the session credential. #1382 — read exact-then-legacy
		// (credsecret.For), and only a Secret the caller owns
		// (endpoints.OwnsSecret): another user's Secret under a shared name
		// is a miss, never a dial with their credential.
		return m.resolveOwn(ctx)
	}

	// A NAMED ref: the Secret it names, read through the ServiceAccount.
	// Ship D.2 (0.30.143) — F-3 cache lookup FIRST. On any soft miss
	// (cache=off, namespace mismatch, pre-sync, Secret absent, snapshot nil
	// pre-readiness) the function returns served=false and we fall through
	// to the upstream plumbing call below — the pre-D.2 behavior preserved
	// exactly (AC-D2.4). A malformed Secret in the snapshot (served=false
	// with an error) falls through too: the apiserver is authoritative, and
	// answers the same verbatim error when the Secret really is malformed.
	if ep, served, _ := FromInformerSecret(ctx, ref.Namespace, ref.Name); served {
		return ep, false, nil
	}

	// Ship D (0.30.141) — F-3: record BEFORE the plumbing call so a
	// panicking secret-GET still increments the counter (AC-D.3). The gvr
	// label is left empty: synthesizing a placeholder would inflate
	// cardinality without diagnostic value.
	sctx := cache.RecordApiserverFallthrough(ctx, cache.ReasonSecretGet, "")
	ep, err := credhttp.FromSecret(sctx, m.rc, ref.Name, ref.Namespace)
	if err != nil {
		return ep, false, err
	}
	// Ship 0.30.165 — normalize CA bytes to the single-base64-encoded
	// PEM shape that plumbing's transport expects (see endpoints_ca.go).
	ep.CertificateAuthorityData = string(normalizeCAData([]byte(ep.CertificateAuthorityData)))
	return ep, false, nil
}

// resolveOwn resolves the caller's OWN credential (#994, #1382): the names
// credsecret.For gives, exact first, from the Secrets informer and then, when
// it holds no Secret the caller owns, from the apiserver
// (own_credential.go). A Secret the caller does not own, under any name, is a
// miss; when no name holds one the error is a NotFound, which fails the stage
// as a missing credential does. The endpoint is never the ServiceAccount's
// (isSAEndpoint=false).
//
// AC-D2.7 — the !env.TestMode ServerURL override applies UNIFORMLY on the
// informer and the apiserver path: the stage dials the in-cluster apiserver
// with the caller's credential. Kept here, not in the readers, per
// feedback_no_special_cases: it is the resolver's contextual decision, not a
// Secret-cache concern.
func (m *endpointReferenceMapper) resolveOwn(ctx context.Context) (endpoints.Endpoint, bool, error) {
	names := credsecret.For(m.username, m.groups)
	// A malformed owned Secret in the snapshot (served=false with an error)
	// falls through to the apiserver, as on the named path: the apiserver is
	// authoritative and answers the same verbatim error when it is malformed.
	ep, served, _ := FromInformerOwnSecret(ctx, m.authnNS, m.username, names)
	if !served {
		var err error
		// Ship D (0.30.141) — F-3: one fallthrough per miss, recorded before
		// the reads, however many names they try.
		sctx := cache.RecordApiserverFallthrough(ctx, cache.ReasonSecretGet, "")
		ep, err = FromAPIServerOwnSecret(sctx, m.rc, m.authnNS, m.username, names)
		if err != nil {
			return ep, false, err
		}
	}
	if !env.TestMode() {
		ep.ServerURL = "https://kubernetes.default.svc"
	}
	return ep, false, nil
}
